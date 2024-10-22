from langchain.llms.base import LLM
from typing import Optional, List, ClassVar
from azure.ai.inference import ChatCompletionsClient
from azure.ai.inference.models import SystemMessage, UserMessage, TextContentItem, ImageContentItem, ImageUrl, ImageDetailLevel
from azure.core.credentials import AzureKeyCredential
from pydantic import PrivateAttr
import os

class AzureInferenceLLM(LLM):
    # Class variables to store API credentials and endpoint
    endpoint: ClassVar[str] = os.getenv("AZURE_INFERENCE_ENDPOINT", "https://Llama-3-2-11B-Vision-Instruct-sx.eastus2.models.ai.azure.com")
    api_key: ClassVar[str] = os.getenv("AZURE_INFERENCE_CREDENTIAL", "")

    # Using PrivateAttr to prevent Pydantic from treating this as a model field
    _client: ChatCompletionsClient = PrivateAttr()

    def __init__(self, **kwargs):
        """Initialize the LLM with Azure client."""
        super().__init__(**kwargs)  # Initialize the base LLM
        self._client = ChatCompletionsClient(
            endpoint=self.endpoint,
            credential=AzureKeyCredential(self.api_key)
        )

    def run_azure_inference(self, prompt: str, image_path: Optional[str] = None) -> str:
        """Directly call the Azure AI inference model with optional image."""
        
        # Create system message
        system_message = SystemMessage(content="You are an AI assistant that describes images in details.")
        
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
        response = self._client.complete(messages=messages)

        # Return the assistant's response
        return response.choices[0].message.content

    def _call(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        """Default text-only method for Langchain compatibility."""
        # Use run_azure_inference for basic text handling
        return self.run_azure_inference(prompt)

    def _llm_type(self) -> str:
        """Return the type of LLM."""
        return "azure_inference_llm"

# Example usage with Langchain

from langchain import PromptTemplate, LLMChain

# Initialize the custom AzureInferenceLLM
llm = AzureInferenceLLM()

# Define the prompt template
template = "Analyze the image and answer the following question: {question}"
prompt = PromptTemplate(template=template, input_variables=["question"])

# Custom function to invoke the AzureInferenceLLM outside of Langchain's `_call` flow
def azure_inference_with_image(llm, question, image_path=None):
    # Call the custom Azure inference method directly for handling images
    return llm.run_azure_inference(prompt=question, image_path=image_path)

# Example call with an image
result = azure_inference_with_image(llm, "Is the person wearing the right safety gear?", image_path="/content/safety2.jpg")

print(result)
