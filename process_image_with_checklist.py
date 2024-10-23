import os
from langchain.llms.base import LLM
from typing import Optional, List, ClassVar
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage, TextContentItem, ImageContentItem, ImageUrl, ImageDetailLevel
from azure.core.credentials import AzureKeyCredential
from pydantic import PrivateAttr
import docx  # Import to work with .docx files

class AzureInferenceLLM(LLM):
    """Custom LLM using Azure's ChatCompletionsClient to handle inference with text and image inputs."""
    
    # Class variables to store API credentials and endpoint
    endpoint: ClassVar[str] = os.getenv("AZURE_INFERENCE_ENDPOINT")
    api_key: ClassVar[str] = os.getenv("AZURE_INFERENCE_CREDENTIAL")

    # Using PrivateAttr to prevent Pydantic from treating this as a model field
    _client: ChatCompletionsClient = PrivateAttr()

    def __init__(self, **kwargs):
        """Initialize the LLM with Azure client."""
        super().__init__(**kwargs)  # Initialize the base LLM

        # Ensure API key and endpoint are properly set
        if not self.api_key:
            raise ValueError("API key is missing. Please set the AZURE_INFERENCE_CREDENTIAL environment variable.")
        if not self.endpoint:
            raise ValueError("Endpoint is missing. Please set the AZURE_INFERENCE_ENDPOINT environment variable.")

        self._client = ChatCompletionsClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.api_key)
        )

    def run_azure_inference(self, prompt: str, image_path: Optional[str] = None) -> str:
        """
        Directly call the Azure AI inference model with optional image.
        
        Args:
            prompt (str): The prompt text to be sent to the Azure inference model.
            image_path (Optional[str]): Path to the image file for analysis. Defaults to None.

        Returns:
            str: The response from the Azure inference model.

        Raises:
            ValueError: If the Azure API call fails or returns an unexpected response.
        """
        # Create system message
        system_message = SystemMessage(content="You are an AI assistant that evaluates safety based on a checklist and image.")
        
        # Create user message content with text
        user_message_content = [TextContentItem(text=prompt)]
        
        # If an image is provided, load it into the message
        if image_path:
            user_message_content.append(
                ImageContentItem(
                    image_url=ImageUrl.load(
                        image_file=image_path,
                        image_format=image_path.split(".")[-1],  # Extract file extension (jpg, png, etc.)
                        detail=ImageDetailLevel.HIGH
                    )
                )
            )
        
        # Create full message list
        messages = [
            system_message,
            UserMessage(content=user_message_content),
        ]

        # Send the request to Azure using the ChatCompletionsClient
        try:
            response = self._client.complete(messages=messages)
        except Exception as e:
            raise ValueError(f"Azure API call failed: {str(e)}")
        
        # Return the assistant's response
        return response.choices[0].message.content

    def evaluate_safety(self, docx_file: str, image_path: str) -> str:
        """
        Evaluate the safety of personnel based on the checklist in the .docx file and the image.

        Args:
            docx_file (str): Path to the .docx file containing the safety checklist.
            image_path (str): Path to the image to evaluate.

        Returns:
            str: The safety rating (1-5) based on the evaluation.
        """
        # Extract checklist from the .docx file
        checklist = self._extract_checklist_from_docx(docx_file)

        # Combine the checklist into a single string for prompting the model
        prompt = f"Based on the following safety checklist:\n\n{checklist}\n\nEvaluate the image provided."
        
        # Get the inference result from Azure
        response = self.run_azure_inference(prompt, image_path=image_path)

        # Based on the response, assign a safety rating (1-5)
        safety_rating = self._generate_safety_rating(response)

        return f"Safety Rating: {safety_rating}/5\nDetails: {response}"

    def _extract_checklist_from_docx(self, docx_file: str) -> str:
        """
        Extract the checklist from a .docx file.

        Args:
            docx_file (str): Path to the .docx file.

        Returns:
            str: The extracted checklist as a string.
        """
        doc = docx.Document(docx_file)
        checklist = "\n".join([para.text for para in doc.paragraphs if para.text.strip()])
        return checklist

    def _generate_safety_rating(self, response: str) -> int:
        """
        Generate a safety rating based on the model's response.

        Args:
            response (str): The response from the Azure inference model.

        Returns:
            int: A safety rating between 1 and 5.
        """
        # Basic logic to generate a safety rating from the model's response
        # (For demonstration, this is based on keywords; you can improve this logic as needed)
        if "completely safe" in response or "all safety measures followed" in response:
            return 5
        elif "mostly safe" in response:
            return 4
        elif "some safety violations" in response:
            return 3
        elif "several safety violations" in response:
            return 2
        else:
            return 1

    def _call(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        """
        Default text-only method for Langchain compatibility.

        Args:
            prompt (str): The input prompt to process.
            stop (Optional[List[str]]): A list of stop words for the model. Defaults to None.

        Returns:
            str: The response from the Azure inference model.
        """
        # Use run_azure_inference for basic text handling
        return self.run_azure_inference(prompt)

    def _llm_type(self) -> str:
        """Return the type of LLM."""
        return "azure_inference_llm"

# Example usage:

# Initialize the custom AzureInferenceLLM
llm = AzureInferenceLLM()

# Example of evaluating an image using a .docx checklist
docx_file_path = "/content/sample_safety_checklist.docx"  # Path to your checklist file
image_file_path = "/content/safety3.jpg"  # Path to the image you want to evaluate

# Perform the safety evaluation
result = llm.evaluate_safety(docx_file_path, image_file_path)

print(result)
